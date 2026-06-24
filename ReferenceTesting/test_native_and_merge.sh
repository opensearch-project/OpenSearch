#!/bin/bash
# Tests native path (no deletes) and merge convergence after soft-delete upgrade
set -e
HOST="localhost:9200"
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'

echo "============================================"
echo "TEST 1: NATIVE PATH (no deletes, 100k docs)"
echo "============================================"

echo ""
echo "=== Cleanup ==="
curl -s -X DELETE "$HOST/ecom_native" > /dev/null 2>&1 || true

echo "=== Create index + ingest 100k ==="
curl -s -X PUT "$HOST/ecom_native" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_native.ndjson', 'w') as f:
    for i in range(100000):
        f.write(json.dumps({'index': {'_index': 'ecom_native'}}) + '\n')
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
TOTAL=$(wc -l < /tmp/test_native.ndjson)
OFF=1; COUNT=0; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_native.ndjson > /tmp/_batch_native.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_native.ndjson > /dev/null
  OFF=$((END + 1))
done
echo "  Ingested 100k docs (no deletes)"

echo "=== Flush ==="
curl -s -X POST "$HOST/ecom_native/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/ecom_native/_refresh" > /dev/null

echo "=== Segments (should have 0 deleted) ==="
curl -s "$HOST/ecom_native/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: {seg[\"num_docs\"]} docs, {seg.get(\"deleted_docs\",0)} deleted')
"

echo "=== Aggregation BEFORE ==="
BEFORE=$(curl -s -X POST "$HOST/ecom_native/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo "=== UPGRADE ==="
RESULT=$(curl -s -X POST "$HOST/ecom_native/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [{"name": "customer_gender"},{"name": "currency"},{"name": "day_of_week"},{"name": "order_date"}],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}')
echo "  $RESULT"

echo "=== Aggregation AFTER ==="
AFTER=$(curl -s -X POST "$HOST/ecom_native/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo "=== COMPARE ==="
python3 -c "
import json, sys
before = json.loads('''$BEFORE''')
after = json.loads('''$AFTER''')
bb = {b['key']: b for b in before['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in bb:
    if key not in ab: print(f'  {key}: MISSING'); all_pass=False; continue
    for name in ['doc_count']:
        bv = bb[key][name]; av = ab[key][name]
        ok = bv == av
        if not ok: all_pass = False
        print(f'  {key}.{name}: {bv} vs {av} → {\"PASS\" if ok else \"FAIL\"}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    print(f'  {key}.revenue: {bv:.2f} vs {av:.2f} → {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('  *** TEST 1 PASSED ***')
else: print('  *** TEST 1 FAILED ***'); sys.exit(1)
"


echo ""
echo "================================================"
echo "TEST 2: MERGE CONVERGENCE (soft deletes → merge)"
echo "================================================"

echo ""
echo "=== Cleanup ==="
curl -s -X DELETE "$HOST/ecom_merge" > /dev/null 2>&1 || true

echo "=== Create index + ingest 100k ==="
curl -s -X PUT "$HOST/ecom_merge" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
TOTAL=$(wc -l < /tmp/test_native.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_native.ndjson | sed 's/ecom_native/ecom_merge/g' > /tmp/_batch_merge.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_merge.ndjson > /dev/null
  OFF=$((END + 1))
done
echo "  Ingested 100k docs"

echo "=== Flush ==="
curl -s -X POST "$HOST/ecom_merge/_flush?force=true" > /dev/null

echo "=== Delete ~10k docs ==="
DEL=$(curl -s -X POST "$HOST/ecom_merge/_delete_by_query" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "total_quantity": { "lte": 2 } } }
}' | python3 -c "import sys,json;print(json.load(sys.stdin).get('deleted',0))")
echo "  Deleted: $DEL docs"

echo "=== Refresh + Flush (apply soft deletes) ==="
curl -s -X POST "$HOST/ecom_merge/_refresh" > /dev/null
curl -s -X POST "$HOST/ecom_merge/_flush?force=true" > /dev/null

echo "=== Segments BEFORE upgrade (should have deletes) ==="
curl -s "$HOST/ecom_merge/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                del_docs = seg.get('deleted_docs', 0)
                marker = ' ***' if del_docs > 0 else ''
                print(f'  {seg_name}: {seg[\"num_docs\"]} docs, {del_docs} deleted{marker}')
"

echo "=== Aggregation BASELINE ==="
BASELINE=$(curl -s -X POST "$HOST/ecom_merge/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo "=== UPGRADE ==="
RESULT=$(curl -s -X POST "$HOST/ecom_merge/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [{"name": "customer_gender"},{"name": "currency"},{"name": "day_of_week"},{"name": "order_date"}],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}')
echo "  $RESULT"

echo "=== Aggregation AFTER upgrade (before merge) ==="
AFTER_UPGRADE=$(curl -s -X POST "$HOST/ecom_merge/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_UPGRADE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo "=== FORCE MERGE (converge to native composite) ==="
curl -s -X POST "$HOST/ecom_merge/_forcemerge?max_num_segments=1&flush=true" > /dev/null
sleep 5
curl -s -X POST "$HOST/ecom_merge/_refresh" > /dev/null

echo "=== Segments AFTER merge ==="
curl -s "$HOST/ecom_merge/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: {seg[\"num_docs\"]} docs, {seg.get(\"deleted_docs\",0)} deleted, compound={seg.get(\"compound\",\"?\")}')
"

echo "=== Aggregation AFTER merge ==="
AFTER_MERGE=$(curl -s -X POST "$HOST/ecom_merge/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_MERGE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo "=== COMPARE (baseline vs after-merge) ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after_merge = json.loads('''$AFTER_MERGE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after_merge['aggregations']['by_gender']['buckets']}
all_pass = True
for key in bb:
    if key not in ab: print(f'  {key}: MISSING'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    print(f'  {key}.doc_count: {bv} vs {av} → {\"PASS\" if ok else \"FAIL\"}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    print(f'  {key}.revenue: {bv:.2f} vs {av:.2f} → {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('  *** TEST 2 PASSED ***')
else: print('  *** TEST 2 FAILED ***'); sys.exit(1)
"

echo ""
echo "========================"
echo "ALL TESTS COMPLETE"
echo "========================"
