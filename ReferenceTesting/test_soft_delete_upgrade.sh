#!/bin/bash
# Tests Bug #7 fix: buildLiveDocsBitset() correctly excludes soft-deleted docs
# Uses _delete_by_query which applies soft-delete DV updates directly to segments
set -e
HOST="localhost:9200"
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'

echo "========================================================"
echo "TEST: SOFT DELETE UPGRADE (Bug #7 validation)"
echo "========================================================"

echo ""
echo "=== Cleanup ==="
curl -s -X DELETE "$HOST/ecom_softdel" > /dev/null 2>&1 || true
sleep 1

echo "=== Step 1: Create index (1 shard, no auto-merge) ==="
curl -s -X PUT "$HOST/ecom_softdel" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.refresh_interval": "-1",
    "index.merge.policy.max_merged_segment": "100gb",
    "index.merge.policy.segments_per_tier": "100"
  },
  "mappings": {
    "properties": {
      "customer_gender": { "type": "keyword" },
      "currency": { "type": "keyword" },
      "day_of_week": { "type": "keyword" },
      "order_date": { "type": "date" },
      "taxful_total_price": { "type": "double" },
      "taxless_total_price": { "type": "double" },
      "total_quantity": { "type": "integer" },
      "total_unique_products": { "type": "integer" },
      "day_of_week_i": { "type": "integer" }
    }
  }
}' | python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  acknowledged={r.get(\"acknowledged\")}')"

echo ""
echo "=== Step 2: Ingest 100k docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/softdel_bulk.ndjson', 'w') as f:
    for i in range(100000):
        f.write(json.dumps({'index': {'_index': 'ecom_softdel', '_id': str(i)}}) + '\n')
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
TOTAL=$(wc -l < /tmp/softdel_bulk.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/softdel_bulk.ndjson > /tmp/_chunk.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_chunk.ndjson > /dev/null
  OFF=$((END + 1))
done
echo "  Ingested 100k docs with explicit _id (0-99999)"

echo "=== Flush ==="
curl -s -X POST "$HOST/ecom_softdel/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/ecom_softdel/_refresh" > /dev/null

echo ""
echo "=== Step 3: Aggregation BEFORE delete (full 100k) ==="
BEFORE_DEL=$(curl -s -X POST "$HOST/ecom_softdel/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE_DEL" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== Step 4: Delete ~10k docs via _delete_by_query ==="
# This uses total_quantity <= 2 which should hit ~10% of docs
DEL_RESP=$(curl -s -X POST "$HOST/ecom_softdel/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "total_quantity": { "lte": 2 } } }
}')
DELETED=$(echo "$DEL_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('deleted',0))")
echo "  Deleted: $DELETED docs"

echo "=== Flush to commit soft deletes ==="
curl -s -X POST "$HOST/ecom_softdel/_flush?force=true" > /dev/null

echo ""
echo "=== Step 5: Verify segments have soft deletes ==="
echo "  Segments:"
curl -s "$HOST/ecom_softdel/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
has_deletes = False
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                del_docs = seg.get('deleted_docs', 0)
                if del_docs > 0:
                    has_deletes = True
                marker = ' ← HAS SOFT DELETES' if del_docs > 0 else ''
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={del_docs}{marker}')
if not has_deletes:
    print('    WARNING: No segments have deletes! Test may not exercise soft-delete path.')
"

echo ""
echo "=== Step 6: Aggregation AFTER delete (baseline for upgrade comparison) ==="
AFTER_DEL=$(curl -s -X POST "$HOST/ecom_softdel/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_DEL" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total} (should be 100000 - $DELETED = $(echo "100000 - $DELETED" | bc))')
"

echo ""
echo "=========================================="
echo "=== Step 7: UPGRADE TO STAR TREE ==="
echo "=========================================="
UPGRADE_RESP=$(curl -s -X POST "$HOST/ecom_softdel/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "  Upgrade response:"
echo "$UPGRADE_RESP" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f'    total_shards={r.get(\"_shards\",{}).get(\"total\")}, successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
except: pass
" 2>/dev/null

echo ""
echo "=== Step 8: Aggregation AFTER upgrade ==="
AFTER_UPGRADE=$(curl -s -X POST "$HOST/ecom_softdel/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_UPGRADE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== Step 9: COMPARE (after-delete baseline vs after-upgrade) ==="
python3 -c "
import json, sys
baseline = json.loads('''$AFTER_DEL''')
after = json.loads('''$AFTER_UPGRADE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    print(f'    {key}.doc_count: {bv} vs {av} → {\"PASS\" if ok else \"FAIL\"}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    print(f'    {key}.revenue: {bv:.2f} vs {av:.2f} → {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('    *** SOFT DELETE UPGRADE TEST: PASSED ***')
else: print('    *** SOFT DELETE UPGRADE TEST: FAILED ***')
    # If FAILED: star tree includes deleted docs (Bug #7 not fixed)
    # Expected: after-upgrade counts should match after-delete counts (not 100k)
"

echo ""
echo "=== Step 10: Force merge + verify convergence ==="
curl -s -X POST "$HOST/ecom_softdel/_forcemerge?max_num_segments=1&flush=true" | python3 -c "
import sys, json; r = json.load(sys.stdin)
print(f'    forcemerge: successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
"
sleep 3
curl -s -X POST "$HOST/ecom_softdel/_refresh" > /dev/null

echo ""
echo "=== Step 11: Segments after merge ==="
curl -s "$HOST/ecom_softdel/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={seg.get(\"deleted_docs\",0)}')
"

echo ""
echo "=== Step 12: Aggregation after merge ==="
AFTER_MERGE=$(curl -s -X POST "$HOST/ecom_softdel/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_MERGE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
print(f'    terminated_early: {d.get(\"terminated_early\")}')
"

echo ""
echo "=== Step 13: COMPARE (after-delete baseline vs after-merge) ==="
python3 -c "
import json, sys
baseline = json.loads('''$AFTER_DEL''')
after_merge = json.loads('''$AFTER_MERGE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after_merge['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    print(f'    {key}.doc_count: {bv} vs {av} → {\"PASS\" if ok else \"FAIL\"}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    print(f'    {key}.revenue: {bv:.2f} vs {av:.2f} → {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('    *** MERGE AFTER SOFT DELETE: PASSED ***')
else: print('    *** MERGE AFTER SOFT DELETE: FAILED ***')
"

echo ""
echo "========================"
echo "ALL TESTS COMPLETE"
echo "========================"
